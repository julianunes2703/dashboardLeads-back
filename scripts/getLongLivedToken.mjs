import axios from 'axios'
import dotenv from 'dotenv'

dotenv.config()

async function getLongLivedToken() {
  try {
    const response = await axios.get(
      'https://graph.facebook.com/v19.0/oauth/access_token',
      {
        params: {
          grant_type: 'fb_exchange_token',
          client_id: process.env.META_APP_ID,
          client_secret: process.env.META_APP_SECRET,
          fb_exchange_token: process.env.META_ACCESS_TOKEN, // token curto
        },
      }
    )

    console.log('✅ Token longo gerado:\n')
    console.log(response.data)

  } catch (error) {
    console.error('❌ Erro ao gerar token:')
    console.error(error.response?.data || error.message)
  }
}

getLongLivedToken()